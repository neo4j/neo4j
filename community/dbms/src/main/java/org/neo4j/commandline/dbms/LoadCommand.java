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

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.common.Database;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;

import static java.util.Objects.requireNonNull;
import static org.neo4j.commandline.Util.canonicalPath;
import static org.neo4j.commandline.Util.checkLock;
import static org.neo4j.commandline.Util.isSameOrChildPath;
import static org.neo4j.commandline.Util.wrapIOException;
import static org.neo4j.commandline.arguments.common.Database.ARG_DATABASE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;

public class LoadCommand implements AdminCommand
{

    private static final Arguments arguments = new Arguments()
            .withArgument( new MandatoryCanonicalPath( "from", "archive-path", "Path to archive created with the " +
                    "dump command." ) )
            .withDatabase()
            .withArgument( new OptionalBooleanArg( "force", false, "If an existing database should be replaced." ) );

    private final Path homeDir;
    private final Path configDir;
    private final Loader loader;
    public LoadCommand( Path homeDir, Path configDir, Loader loader )
    {
        requireNonNull(homeDir);
        requireNonNull( configDir );
        requireNonNull( loader );
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.loader = loader;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        arguments.parse( args );
        Path archive = arguments.getMandatoryPath( "from" );
        String database = arguments.get( ARG_DATABASE );
        boolean force = arguments.getBoolean( "force" );

        Config config = buildConfig( database );

        Path databaseDirectory = canonicalPath( getDatabaseDirectory( config ) );
        Path transactionLogsDirectory = canonicalPath( getTransactionalLogsDirectory( config ) );

        deleteIfNecessary( databaseDirectory, transactionLogsDirectory, force );
        load( archive, database, databaseDirectory, transactionLogsDirectory );
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

    private void deleteIfNecessary( Path databaseDirectory, Path transactionLogsDirectory, boolean force ) throws CommandFailed
    {
        try
        {
            if ( force )
            {
                checkLock( databaseDirectory );
                FileUtils.deletePathRecursively( databaseDirectory );
                if ( !isSameOrChildPath( databaseDirectory, transactionLogsDirectory ) )
                {
                    FileUtils.deletePathRecursively( transactionLogsDirectory );
                }
            }
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private void load( Path archive, String database, Path databaseDirectory, Path transactionLogsDirectory ) throws CommandFailed
    {
        try
        {
            loader.load( archive, databaseDirectory, transactionLogsDirectory );
        }
        catch ( NoSuchFileException e )
        {
            if ( Paths.get( e.getMessage() ).toAbsolutePath().equals( archive.toAbsolutePath() ) )
            {
                throw new CommandFailed( "archive does not exist: " + archive, e );
            }
            wrapIOException( e );
        }
        catch ( FileAlreadyExistsException e )
        {
            throw new CommandFailed( "database already exists: " + database, e );
        }
        catch ( AccessDeniedException e )
        {
            throw new CommandFailed(
                    "you do not have permission to load a database -- is Neo4j running as a " + "different user?", e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
        catch ( IncorrectFormat incorrectFormat )
        {
            throw new CommandFailed( "Not a valid Neo4j archive: " + archive, incorrectFormat );
        }
    }

    public static Arguments arguments()
    {
        return arguments;
    }
}
