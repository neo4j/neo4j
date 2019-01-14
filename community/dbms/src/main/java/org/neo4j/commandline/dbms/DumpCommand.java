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
package org.neo4j.commandline.dbms;

import java.io.Closeable;
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
import org.neo4j.commandline.arguments.common.Database;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.StoreLocker;

import static java.lang.String.format;
import static org.neo4j.commandline.Util.canonicalPath;
import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;

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
        Path databaseDirectory = canonicalPath( getDatabaseDirectory( config ) );
        Path transactionLogsDirectory = canonicalPath( getTransactionalLogsDirectory( config ) );

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( databaseDirectory.toFile() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailed( "database does not exist: " + database, e );
        }

        try ( Closeable ignored = StoreLockChecker.check( databaseDirectory ) )
        {
            dump( database, databaseDirectory, transactionLogsDirectory, archive );
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

    private Path getDatabaseDirectory( Config config )
    {
        return config.get( database_path ).toPath();
    }

    private Path getTransactionalLogsDirectory( Config config )
    {
        return config.get( logical_logs_location ).toPath();
    }

    private Config buildConfig( String databaseName )
    {
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withConnectorsDisabled()
                .withSetting( GraphDatabaseSettings.active_database, databaseName )
                .build();
    }

    private Path calculateArchive( String database, Path to )
    {
        return Files.isDirectory( to ) ? to.resolve( database + ".dump" ) : to;
    }

    private void dump( String database, Path databaseDirectory, Path transactionalLogsDirectory, Path archive )
            throws CommandFailed
    {
        try
        {
            dumper.dump( databaseDirectory, transactionalLogsDirectory, archive, this::isStoreLock );
        }
        catch ( FileAlreadyExistsException e )
        {
            throw new CommandFailed( "archive already exists: " + e.getMessage(), e );
        }
        catch ( NoSuchFileException e )
        {
            if ( Paths.get( e.getMessage() ).toAbsolutePath().equals( databaseDirectory.toAbsolutePath() ) )
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

    private boolean isStoreLock( Path path )
    {
        return Objects.equals( path.getFileName().toString(), StoreLocker.STORE_LOCK_FILENAME );
    }

    private void wrapIOException( IOException e ) throws CommandFailed
    {
        throw new CommandFailed(
                format( "unable to dump database: %s: %s", e.getClass().getSimpleName(), e.getMessage() ), e );
    }

    public static Arguments arguments()
    {
        return arguments;
    }
}
