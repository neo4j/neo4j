/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.internal.StoreLocker;
import org.neo4j.server.configuration.ConfigLoader;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.Converters.identity;
import static org.neo4j.kernel.impl.util.Converters.mandatory;

public class LoadCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "load" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "--from=<archive-path> --database=<database> [--force]" );
        }

        @Override
        public String description()
        {
            return "Load a database from an archive. <archive-path> must be an archive created with the dump " +
                    "command. <database> is the name of the database to create. Existing databases can be replaced " +
                    "by specifying --force. It is not possible to replace a database that is mounted in a running " +
                    "Neo4j server.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new LoadCommand( homeDir, configDir, new Loader() );
        }
    }

    private final Path homeDir;
    private final Path configDir;
    private final Loader loader;

    public LoadCommand( Path homeDir, Path configDir, Loader loader )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.loader = loader;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        Path archive = parse( args, "from", Paths::get );
        String database = parse( args, "database", identity() );
        boolean force = Args.parse( args ).getBoolean( "force" );

        Path databaseDirectory = toDatabaseDirectory( database );

        deleteIfNecessary( databaseDirectory, force );
        load( archive, database, databaseDirectory );
    }

    private <T> T parse( String[] args, String argument, Function<String, T> converter ) throws IncorrectUsage
    {
        try
        {
            return Args.parse( args ).interpretOption( argument, mandatory(), converter );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
    }

    private Path toDatabaseDirectory( String databaseName )
    {
        //noinspection unchecked
        return new ConfigLoader( asList( DatabaseManagementSystemSettings.class, GraphDatabaseSettings.class ) )
                .loadOfflineConfig(
                        Optional.of( homeDir.toFile() ),
                        Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) )
                .with( stringMap( DatabaseManagementSystemSettings.active_database.name(), databaseName ) )
                .get( database_path ).toPath();
    }

    private void deleteIfNecessary( Path databaseDirectory, boolean force ) throws CommandFailed
    {
        try
        {
            if ( force )
            {
                checkLock( databaseDirectory );
                FileUtils.deletePathRecursively( databaseDirectory );
            }
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private void checkLock( Path databaseDirectory ) throws CommandFailed
    {
        try
        {
            StoreLocker storeLocker = new StoreLocker( new DefaultFileSystemAbstraction() );
            storeLocker.checkLock( databaseDirectory.toFile() );
            storeLocker.release();
        }
        catch ( StoreLockException e )
        {
            throw new CommandFailed( "the database is in use -- stop Neo4j and try again", e );
        }
        catch ( IOException e )
        {
            wrapIOException( e );
        }
    }

    private void load( Path archive, String database, Path databaseDirectory ) throws CommandFailed
    {
        try
        {
            loader.load( archive, databaseDirectory );
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
            throw new CommandFailed( "you do not have permission to load a database -- is Neo4j running as a " +
                    "different user?", e );
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

    private void wrapIOException( IOException e ) throws CommandFailed
    {
        throw new CommandFailed( format( "unable to load database: %s: %s",
                e.getClass().getSimpleName(), e.getMessage() ), e );
    }
}
