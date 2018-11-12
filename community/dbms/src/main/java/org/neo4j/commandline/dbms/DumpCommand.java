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
package org.neo4j.commandline.dbms;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Validators;

import static java.lang.String.format;
import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.configuration.LayoutConfig.of;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;

public class DumpCommand implements AdminCommand
{

    private static final Arguments arguments = new Arguments()
            .withDatabase()
            .withTo( "Destination (file or folder) of database dump." );

    private final Path homeDir;
    private final Path configDir;
    private final Dumper dumper;

    public DumpCommand( Path homeDir, Path configDir, Dumper dumper )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.dumper = dumper;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        String database = arguments.parse( args ).get( ARG_DATABASE );
        Path archive = calculateArchive( database, arguments.getMandatoryPath( "to" ) );

        Config config = buildConfig( database );
        Path databaseDirectory = getDatabaseDirectory( config );
        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDirectory.toFile(), of( config ) );

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( databaseLayout.databaseDirectory() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailed( "database does not exist: " + database, e );
        }

        try ( Closeable ignored = StoreLockChecker.check( databaseLayout.getStoreLayout() ) )
        {
            checkDbState( databaseLayout, config );
            dump( database, databaseLayout, archive );
        }
        catch ( StoreLockException e )
        {
            throw new CommandFailed( "the database is in use -- stop Neo4j and try again", e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
        catch ( CannotWriteException e )
        {
            throw new CommandFailed( "you do not have permission to dump the database -- is Neo4j running as a different user?", e );
        }
    }

    private static Path getDatabaseDirectory( Config config )
    {
        return config.get( database_path ).toPath();
    }

    private Config buildConfig( String databaseName )
    {
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withConnectorsDisabled()
                .withNoThrowOnFileLoadFailure()
                .withSetting( GraphDatabaseSettings.active_database, databaseName )
                .build();
    }

    private static Path calculateArchive( String database, Path to )
    {
        return Files.isDirectory( to ) ? to.resolve( database + ".dump" ) : to;
    }

    private void dump( String database, DatabaseLayout databaseLayout, Path archive )
            throws CommandFailed
    {
        Path databasePath = databaseLayout.databaseDirectory().toPath();
        try
        {
            File storeLockFile = databaseLayout.getStoreLayout().storeLockFile();
            dumper.dump( databasePath, databaseLayout.getTransactionLogsDirectory().toPath(), archive,
                    path -> Objects.equals( path.getFileName().toString(), storeLockFile.getName() ) );
        }
        catch ( FileAlreadyExistsException e )
        {
            throw new CommandFailed( "archive already exists: " + e.getMessage(), e );
        }
        catch ( NoSuchFileException e )
        {
            if ( Paths.get( e.getMessage() ).toAbsolutePath().equals( databasePath ) )
            {
                throw new CommandFailed( "database does not exist: " + database, e );
            }
            wrapIOException( e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private static void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration ) throws CommandFailed
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration ) )
        {
            throw new CommandFailed( joinAsLines( "Active logical log detected, this might be a source of inconsistencies.",
                    "Please recover database before running the dump.",
                    "To perform recovery please start database and perform clean shutdown." ) );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration ) throws CommandFailed
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration );
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Failure when checking for recovery state: '%s'." + e.getMessage(), e );
        }
    }

    private static void wrapIOException( IOException e ) throws CommandFailed
    {
        throw new CommandFailed(
                format( "unable to dump database: %s: %s", e.getClass().getSimpleName(), e.getMessage() ), e );
    }

    public static Arguments arguments()
    {
        return arguments;
    }
}
